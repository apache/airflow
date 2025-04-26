# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
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
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        # TODO: Remove once provider drops support for Airflow 2
        from airflow.utils.context import Context


class TableauJobStatusSensor(BaseSensorOperator):
    """
    Watches the status of a Tableau Server Job.

    Monitors the job until completion and provides detailed logs including the object type and name.

    .. seealso:: https://tableau.github.io/server-client-python/docs/api-ref#jobs

    :param job_id: ID of the job to monitor.
    :param site_id: ID of the Tableau site.
    :param tableau_conn_id: Airflow connection ID for Tableau.
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
        Polls the Tableau Server for the job status.

        Logs object type and object name for better traceability.
        Raises an exception if the job fails or is canceled.
        """
        with TableauHook(self.site_id, self.tableau_conn_id) as tableau_hook:
            job_info = tableau_hook.get_job(self.job_id)

            object_type = job_info.get("type", "UnknownType")
            object_name = job_info.get("object_name", "UnknownName")
            object_id = job_info.get("object_id", self.job_id)
            finish_code = TableauJobFinishCode(job_info.get("finish_code", -1))

            self.log.info(
                "Monitoring Tableau %s '%s' (id: %s). Current finishCode: %s (%s)",
                object_type, object_name, object_id, finish_code.name, finish_code.value
            )

            if finish_code in (TableauJobFinishCode.ERROR, TableauJobFinishCode.CANCELED):
                raise TableauJobFailedException(
                    f"The Tableau {object_type} refresh job for '{object_name}' (id: {object_id}) failed!"
                )

            return finish_code == TableauJobFinishCode.SUCCESS
