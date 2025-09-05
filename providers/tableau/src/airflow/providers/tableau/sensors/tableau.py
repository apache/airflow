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
from airflow.providers.tableau.version_compat import BaseSensorOperator

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
            job_details = tableau_hook.get_job_details(job_id=self.job_id)
            finish_code: TableauJobFinishCode = job_details["finish_code"]
            job_type: str = job_details["job_type"]
            object_name: str | None = job_details["object_name"]
            object_id: str | None = job_details["object_id"]

            # Efficient object descriptor construction
            object_descriptor = (
                f"{object_name} ({object_id})"
                if object_name and object_id
                else object_id or f"job {self.job_id}"
            )

            # Preserve detailed logging
            self.log.info(
                "Job type '%s' on %s has finishCode %s (%s).",
                job_type,
                object_descriptor,
                finish_code.name,
                finish_code.value,
            )

            # Streamlined error handling
            if finish_code in (TableauJobFinishCode.ERROR, TableauJobFinishCode.CANCELED):
                raise TableauJobFailedException(f"The Tableau Refresh Job for {object_descriptor} failed!")

            return finish_code == TableauJobFinishCode.SUCCESS
