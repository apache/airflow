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

import time
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseSensorOperator
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunException, DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.triggers.dbt import DbtCloudRunJobTrigger
from airflow.providers.dbt.cloud.utils.openlineage import generate_openlineage_events_from_dbt_cloud_run

if TYPE_CHECKING:
    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.utils.context import Context


class DbtCloudJobRunSensor(BaseSensorOperator):
    """
    Checks the status of a dbt Cloud job run.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:DbtCloudJobRunSensor`

    :param dbt_cloud_conn_id: The connection identifier for connecting to dbt Cloud.
    :param run_id: The job run identifier.
    :param account_id: The dbt Cloud account identifier.
    :param deferrable: Run sensor in the deferrable mode.
    """

    template_fields = ("dbt_cloud_conn_id", "run_id", "account_id")

    def __init__(
        self,
        *,
        dbt_cloud_conn_id: str = DbtCloudHook.default_conn_name,
        run_id: int,
        account_id: int | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        hook_params: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        if deferrable:
            if "poke_interval" not in kwargs:
                kwargs["poke_interval"] = 5

                if "timeout" not in kwargs:
                    kwargs["timeout"] = 60 * 60 * 24 * 7

        super().__init__(**kwargs)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.run_id = run_id
        self.account_id = account_id
        self.hook_params = hook_params or {}
        self.deferrable = deferrable

    @cached_property
    def hook(self):
        """Returns DBT Cloud hook."""
        return DbtCloudHook(self.dbt_cloud_conn_id, **self.hook_params)

    def poke(self, context: Context) -> bool:
        job_run_status = self.hook.get_job_run_status(run_id=self.run_id, account_id=self.account_id)

        if job_run_status == DbtCloudJobRunStatus.ERROR.value:
            message = f"Job run {self.run_id} has failed."
            raise DbtCloudJobRunException(message)

        if job_run_status == DbtCloudJobRunStatus.CANCELLED.value:
            message = f"Job run {self.run_id} has been cancelled."
            raise DbtCloudJobRunException(message)

        return job_run_status == DbtCloudJobRunStatus.SUCCESS.value

    def execute(self, context: Context) -> None:
        """
        Run the sensor.

        Depending on whether ``deferrable`` is set, this would either defer to
        the triggerer or poll for states of the job run, until the job reaches a
        failure state or success state.
        """
        if not self.deferrable:
            super().execute(context)
        else:
            end_time = time.time() + self.timeout
            if not self.poke(context=context):
                self.defer(
                    timeout=self.execution_timeout,
                    trigger=DbtCloudRunJobTrigger(
                        run_id=self.run_id,
                        conn_id=self.dbt_cloud_conn_id,
                        account_id=self.account_id,
                        poll_interval=self.poke_interval,
                        end_time=end_time,
                        hook_params=self.hook_params,
                    ),
                    method_name="execute_complete",
                )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> int:
        """
        Execute when the trigger fires - returns immediately.

        This relies on trigger to throw an exception, otherwise it assumes
        execution was successful.
        """
        if event["status"] in ["error", "cancelled"]:
            raise AirflowException()
        self.log.info(event["message"])
        return int(event["run_id"])

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage:
        """Implement _on_complete because job_run needs to be triggered first in execute method."""
        return generate_openlineage_events_from_dbt_cloud_run(operator=self, task_instance=task_instance)
