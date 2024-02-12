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

from typing import Any, AsyncIterator, Sequence

from google.cloud.aiplatform_v1 import HyperparameterTuningJob, JobState

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.vertex_ai.hyperparameter_tuning_job import (
    HyperparameterTuningJobAsyncHook,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent


class CreateHyperparameterTuningJobTrigger(BaseTrigger):
    """CreateHyperparameterTuningJobTrigger run on the trigger worker to perform create operation."""

    statuses_success = {
        JobState.JOB_STATE_PAUSED,
        JobState.JOB_STATE_SUCCEEDED,
    }

    def __init__(
        self,
        conn_id: str,
        project_id: str,
        location: str,
        job_id: str,
        poll_interval: int,
        impersonation_chain: str | Sequence[str] | None = None,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.project_id = project_id
        self.location = location
        self.job_id = job_id
        self.poll_interval = poll_interval
        self.impersonation_chain = impersonation_chain

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.google.cloud.triggers.vertex_ai.CreateHyperparameterTuningJobTrigger",
            {
                "conn_id": self.conn_id,
                "project_id": self.project_id,
                "location": self.location,
                "job_id": self.job_id,
                "poll_interval": self.poll_interval,
                "impersonation_chain": self.impersonation_chain,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = self._get_async_hook()
        try:
            job = await hook.wait_hyperparameter_tuning_job(
                project_id=self.project_id,
                location=self.location,
                job_id=self.job_id,
                poll_interval=self.poll_interval,
            )
        except AirflowException as ex:
            yield TriggerEvent({
                "status": "error",
                "message": str(ex),
            })
            return

        status = "success" if job.state in self.statuses_success else "error"
        message = f"Hyperparameter tuning job {job.name} completed with status {job.state.name}"
        yield TriggerEvent({
            "status": status,
            "message": message,
            "job": HyperparameterTuningJob.to_dict(job),
        })

    def _get_async_hook(self) -> HyperparameterTuningJobAsyncHook:
        return HyperparameterTuningJobAsyncHook(
            gcp_conn_id=self.conn_id, impersonation_chain=self.impersonation_chain
        )
