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

from collections.abc import AsyncIterator, Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from google.cloud.aiplatform_v1 import (
    BatchPredictionJob,
    HyperparameterTuningJob,
    JobState,
    PipelineState,
    types,
)

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.vertex_ai.batch_prediction_job import BatchPredictionJobAsyncHook
from airflow.providers.google.cloud.hooks.vertex_ai.custom_job import CustomJobAsyncHook
from airflow.providers.google.cloud.hooks.vertex_ai.hyperparameter_tuning_job import (
    HyperparameterTuningJobAsyncHook,
)
from airflow.providers.google.cloud.hooks.vertex_ai.pipeline_job import PipelineJobAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from proto import Message


class BaseVertexAIJobTrigger(BaseTrigger):
    """
    Base class for Vertex AI job triggers.

    This trigger polls the Vertex AI job and checks its status.

    In order to use it properly, you must:
    - implement the following methods `_wait_job()`.
    - override required `job_type_verbose_name` attribute to provide meaningful message describing your
    job type.
    - override required `job_serializer_class` attribute to provide proto.Message class that will be used
    to serialize your job with `to_dict()` class method.
    """

    job_type_verbose_name: str = "Vertex AI Job"
    job_serializer_class: Message = None

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
        self.trigger_class_path = (
            f"airflow.providers.google.cloud.triggers.vertex_ai.{self.__class__.__name__}"
        )

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.trigger_class_path,
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
        try:
            job = await self._wait_job()
        except AirflowException as ex:
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": str(ex),
                }
            )
            return

        status = "success" if job.state in self.statuses_success else "error"
        message = f"{self.job_type_verbose_name} {job.name} completed with status {job.state.name}"
        yield TriggerEvent(
            {
                "status": status,
                "message": message,
                "job": self._serialize_job(job),
            }
        )

    async def _wait_job(self) -> Any:
        """Awaits a Vertex AI job instance for a status examination."""
        raise NotImplementedError

    def _serialize_job(self, job: Any) -> Any:
        return self.job_serializer_class.to_dict(job)


class CreateHyperparameterTuningJobTrigger(BaseVertexAIJobTrigger):
    """CreateHyperparameterTuningJobTrigger run on the trigger worker to perform create operation."""

    job_type_verbose_name = "Hyperparameter Tuning Job"
    job_serializer_class = HyperparameterTuningJob

    @cached_property
    def async_hook(self) -> HyperparameterTuningJobAsyncHook:
        return HyperparameterTuningJobAsyncHook(
            gcp_conn_id=self.conn_id, impersonation_chain=self.impersonation_chain
        )

    async def _wait_job(self) -> types.HyperparameterTuningJob:
        job: types.HyperparameterTuningJob = await self.async_hook.wait_hyperparameter_tuning_job(
            project_id=self.project_id,
            location=self.location,
            job_id=self.job_id,
            poll_interval=self.poll_interval,
        )
        return job


class CreateBatchPredictionJobTrigger(BaseVertexAIJobTrigger):
    """CreateBatchPredictionJobTrigger run on the trigger worker to perform create operation."""

    job_type_verbose_name = "Batch Prediction Job"
    job_serializer_class = BatchPredictionJob

    @cached_property
    def async_hook(self) -> BatchPredictionJobAsyncHook:
        return BatchPredictionJobAsyncHook(
            gcp_conn_id=self.conn_id, impersonation_chain=self.impersonation_chain
        )

    async def _wait_job(self) -> types.BatchPredictionJob:
        job: types.BatchPredictionJob = await self.async_hook.wait_batch_prediction_job(
            project_id=self.project_id,
            location=self.location,
            job_id=self.job_id,
            poll_interval=self.poll_interval,
        )
        return job


class RunPipelineJobTrigger(BaseVertexAIJobTrigger):
    """Make async calls to Vertex AI to check the state of a Pipeline Job."""

    job_type_verbose_name = "Pipeline Job"
    job_serializer_class = types.PipelineJob
    statuses_success = {
        PipelineState.PIPELINE_STATE_PAUSED,
        PipelineState.PIPELINE_STATE_SUCCEEDED,
    }

    @cached_property
    def async_hook(self) -> PipelineJobAsyncHook:
        return PipelineJobAsyncHook(gcp_conn_id=self.conn_id, impersonation_chain=self.impersonation_chain)

    async def _wait_job(self) -> types.PipelineJob:
        job: types.PipelineJob = await self.async_hook.wait_for_pipeline_job(
            project_id=self.project_id,
            location=self.location,
            job_id=self.job_id,
            poll_interval=self.poll_interval,
        )
        return job


class CustomTrainingJobTrigger(BaseVertexAIJobTrigger):
    """
    Make async calls to Vertex AI to check the state of a running custom training job.

    Return the job when it enters a completed state.
    """

    job_type_verbose_name = "Custom Training Job"
    job_serializer_class = types.TrainingPipeline
    statuses_success = {
        PipelineState.PIPELINE_STATE_PAUSED,
        PipelineState.PIPELINE_STATE_SUCCEEDED,
    }

    @cached_property
    def async_hook(self) -> CustomJobAsyncHook:
        return CustomJobAsyncHook(
            gcp_conn_id=self.conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    async def _wait_job(self) -> types.TrainingPipeline:
        pipeline: types.TrainingPipeline = await self.async_hook.wait_for_training_pipeline(
            project_id=self.project_id,
            location=self.location,
            pipeline_id=self.job_id,
            poll_interval=self.poll_interval,
        )
        return pipeline


class CustomContainerTrainingJobTrigger(BaseVertexAIJobTrigger):
    """
    Make async calls to Vertex AI to check the state of a running custom container training job.

    Return the job when it enters a completed state.
    """

    job_type_verbose_name = "Custom Container Training Job"
    job_serializer_class = types.TrainingPipeline
    statuses_success = {
        PipelineState.PIPELINE_STATE_PAUSED,
        PipelineState.PIPELINE_STATE_SUCCEEDED,
    }

    @cached_property
    def async_hook(self) -> CustomJobAsyncHook:
        return CustomJobAsyncHook(
            gcp_conn_id=self.conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    async def _wait_job(self) -> types.TrainingPipeline:
        pipeline: types.TrainingPipeline = await self.async_hook.wait_for_training_pipeline(
            project_id=self.project_id,
            location=self.location,
            pipeline_id=self.job_id,
            poll_interval=self.poll_interval,
        )
        return pipeline


class CustomPythonPackageTrainingJobTrigger(BaseVertexAIJobTrigger):
    """
    Make async calls to Vertex AI to check the state of a running custom python package training job.

    Return the job when it enters a completed state.
    """

    job_type_verbose_name = "Custom Python Package Training Job"
    job_serializer_class = types.TrainingPipeline
    statuses_success = {
        PipelineState.PIPELINE_STATE_PAUSED,
        PipelineState.PIPELINE_STATE_SUCCEEDED,
    }

    @cached_property
    def async_hook(self) -> CustomJobAsyncHook:
        return CustomJobAsyncHook(
            gcp_conn_id=self.conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    async def _wait_job(self) -> types.TrainingPipeline:
        pipeline: types.TrainingPipeline = await self.async_hook.wait_for_training_pipeline(
            project_id=self.project_id,
            location=self.location,
            pipeline_id=self.job_id,
            poll_interval=self.poll_interval,
        )
        return pipeline
