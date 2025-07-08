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

from airflow.providers.tableau.hooks.tableau import (
    TableauHook,
    TableauJobFailedException,
    TableauJobFinishCode,
)
from airflow.providers.tableau.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import BaseSensorOperator
else:
    from airflow.sensors.base import BaseSensorOperator  # type: ignore[no-redef]

if TYPE_CHECKING:
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        # TODO: Remove once provider drops support for Airflow 2
        from airflow.utils.context import Context


class TableauJobStatusSensor(BaseSensorOperator):
    """
    Watches the status of a Tableau Server Job.

    .. seealso:: https://tableau.github.io/server-client-python/docs/api-ref#jobs

    :param job_id: Id of the job to watch.
    :param site_id: The id of the site where the workbook belongs to.
    :param tableau_conn_id: The :ref:`Tableau Connection id <howto/connection:tableau>`
        containing the credentials to authenticate to the Tableau Server.
    :param max_status_retries: How often to rerun get_job_status in case
        of an error or cancellation, to account for transient errors.
    """

    template_fields: Sequence[str] = ("job_id",)

    def __init__(
        self,
        *,
        job_id: str,
        site_id: str | None = None,
        tableau_conn_id: str = "tableau_default",
        max_status_retries: int = 0,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.tableau_conn_id = tableau_conn_id
        self.job_id = job_id
        self.site_id = site_id
        self.max_status_retries = max_status_retries
        self._retry_attempts = 0

    def poke(self, context: Context) -> bool:
        """
        Pokes until the job has successfully finished.

        When max_status_retries is set, the Sensor will retry
        in case of a TableauJobFinishCode.ERROR, TableauJobFinishCode.CANCELED before raising an Error.

        :param context: The task context during execution.
        :return: True if it succeeded and False if not.
        """
        with TableauHook(self.site_id, self.tableau_conn_id) as tableau_hook:
            finish_code = tableau_hook.get_job_status(job_id=self.job_id)
            self.log.info("Current finishCode is %s (%s)", finish_code.name, finish_code.value)

            if finish_code in (TableauJobFinishCode.ERROR, TableauJobFinishCode.CANCELED):
                if self._retry_attempts < self.max_status_retries:
                    self.log.info("Retrying to get the job status")
                    self._retry_attempts += 1
                    return False
                message = "The Tableau Refresh Workbook Job failed!"
                raise TableauJobFailedException(message)

            return finish_code == TableauJobFinishCode.SUCCESS
