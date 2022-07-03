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

from typing import TYPE_CHECKING

from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunException, DbtCloudJobRunStatus
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
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
    """

    template_fields = ("dbt_cloud_conn_id", "run_id", "account_id")

    def __init__(
        self,
        *,
        dbt_cloud_conn_id: str = DbtCloudHook.default_conn_name,
        run_id: int,
        account_id: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.run_id = run_id
        self.account_id = account_id

    def poke(self, context: Context) -> bool:
        hook = DbtCloudHook(self.dbt_cloud_conn_id)
        job_run_status = hook.get_job_run_status(run_id=self.run_id, account_id=self.account_id)

        if job_run_status == DbtCloudJobRunStatus.ERROR.value:
            raise DbtCloudJobRunException(f"Job run {self.run_id} has failed.")

        if job_run_status == DbtCloudJobRunStatus.CANCELLED.value:
            raise DbtCloudJobRunException(f"Job run {self.run_id} has been cancelled.")

        return job_run_status == DbtCloudJobRunStatus.SUCCESS.value
