#
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
"""This module contains a Dataprep Job sensor."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import BaseSensorOperator
from airflow.providers.google.cloud.hooks.dataprep import GoogleDataprepHook, JobGroupStatuses

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class DataprepJobGroupIsFinishedSensor(BaseSensorOperator):
    """
    Check the status of the Dataprep task to be finished.

    :param job_group_id: ID of the job group to check
    """

    template_fields: Sequence[str] = ("job_group_id",)

    def __init__(
        self,
        *,
        job_group_id: int | str,
        dataprep_conn_id: str = "dataprep_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_group_id = job_group_id
        self.dataprep_conn_id = dataprep_conn_id

    def poke(self, context: Context) -> bool:
        hooks = GoogleDataprepHook(dataprep_conn_id=self.dataprep_conn_id)
        status = hooks.get_job_group_status(job_group_id=int(self.job_group_id))
        return status != JobGroupStatuses.IN_PROGRESS
