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

import asyncio
from collections import Counter
from enum import IntEnum
from functools import cached_property
from typing import Any, AsyncIterator

from botocore.exceptions import WaiterError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.utils.waiter_with_logging import async_wait
from airflow.triggers.base import BaseTrigger, TriggerEvent


class SageMakerTrigger(BaseTrigger):
    """
    SageMakerTrigger is fired as deferred class with params to run the task in triggerer.

    :param job_name: name of the job to check status
    :param job_type: Type of the sagemaker job whether it is Transform or Training
    :param poke_interval:  polling period in seconds to check for the status
    :param max_attempts: Number of times to poll for query state before returning the current state,
        defaults to None.
    :param aws_conn_id: AWS connection ID for sagemaker
    """

    def __init__(
        self,
        job_name: str,
        job_type: str,
        poke_interval: int = 30,
        max_attempts: int = 480,
        aws_conn_id: str | None = "aws_default",
    ):
        super().__init__()
        self.job_name = job_name
        self.job_type = job_type
        self.poke_interval = poke_interval
        self.max_attempts = max_attempts
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize SagemakerTrigger arguments and classpath."""
        return (
            "airflow.providers.amazon.aws.triggers.sagemaker.SageMakerTrigger",
            {
                "job_name": self.job_name,
                "job_type": self.job_type,
                "poke_interval": self.poke_interval,
                "max_attempts": self.max_attempts,
                "aws_conn_id": self.aws_conn_id,
            },
        )

    @cached_property
    def hook(self) -> SageMakerHook:
        return SageMakerHook(aws_conn_id=self.aws_conn_id)

    @staticmethod
    def _get_job_type_waiter(job_type: str) -> str:
        return {
            "training": "TrainingJobComplete",
            "transform": "TransformJobComplete",
            "processing": "ProcessingJobComplete",
            "tuning": "TuningJobComplete",
            "endpoint": "endpoint_in_service",  # this one is provided by boto
        }[job_type.lower()]

    @staticmethod
    def _get_waiter_arg_name(job_type: str) -> str:
        return {
            "training": "TrainingJobName",
            "transform": "TransformJobName",
            "processing": "ProcessingJobName",
            "tuning": "HyperParameterTuningJobName",
            "endpoint": "EndpointName",
        }[job_type.lower()]

    @staticmethod
    def _get_response_status_key(job_type: str) -> str:
        return {
            "training": "TrainingJobStatus",
            "transform": "TransformJobStatus",
            "processing": "ProcessingJobStatus",
            "tuning": "HyperParameterTuningJobStatus",
            "endpoint": "EndpointStatus",
        }[job_type.lower()]

    async def run(self):
        self.log.info("job name is %s and job type is %s", self.job_name, self.job_type)
        async with self.hook.async_conn as client:
            waiter = self.hook.get_waiter(
                self._get_job_type_waiter(self.job_type), deferrable=True, client=client
            )
            await async_wait(
                waiter=waiter,
                waiter_delay=self.poke_interval,
                waiter_max_attempts=self.max_attempts,
                args={self._get_waiter_arg_name(self.job_type): self.job_name},
                failure_message=f"Error while waiting for {self.job_type} job",
                status_message=f"{self.job_type} job not done yet",
                status_args=[self._get_response_status_key(self.job_type)],
            )
            yield TriggerEvent(
                {
                    "status": "success",
                    "message": "Job completed.",
                    "job_name": self.job_name,
                }
            )


class SageMakerPipelineTrigger(BaseTrigger):
    """Trigger to wait for a sagemaker pipeline execution to finish."""

    class Type(IntEnum):
        """Type of waiter to use."""

        COMPLETE = 1
        STOPPED = 2

    def __init__(
        self,
        waiter_type: Type,
        pipeline_execution_arn: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str | None,
    ):
        self.waiter_type = waiter_type
        self.pipeline_execution_arn = pipeline_execution_arn
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "waiter_type": self.waiter_type.value,  # saving the int value here
                "pipeline_execution_arn": self.pipeline_execution_arn,
                "waiter_delay": self.waiter_delay,
                "waiter_max_attempts": self.waiter_max_attempts,
                "aws_conn_id": self.aws_conn_id,
            },
        )

    _waiter_name = {
        Type.COMPLETE: "PipelineExecutionComplete",
        Type.STOPPED: "PipelineExecutionStopped",
    }

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = SageMakerHook(aws_conn_id=self.aws_conn_id)
        async with hook.async_conn as conn:
            waiter = hook.get_waiter(
                self._waiter_name[self.waiter_type], deferrable=True, client=conn
            )
            for _ in range(self.waiter_max_attempts):
                try:
                    await waiter.wait(
                        PipelineExecutionArn=self.pipeline_execution_arn,
                        WaiterConfig={"MaxAttempts": 1},
                    )
                    # we reach this point only if the waiter met a success criteria
                    yield TriggerEvent(
                        {"status": "success", "value": self.pipeline_execution_arn}
                    )
                    return
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        raise

                    self.log.info(
                        "Status of the pipeline execution: %s",
                        error.last_response["PipelineExecutionStatus"],
                    )

                    res = await conn.list_pipeline_execution_steps(
                        PipelineExecutionArn=self.pipeline_execution_arn
                    )
                    count_by_state = Counter(
                        s["StepStatus"] for s in res["PipelineExecutionSteps"]
                    )
                    running_steps = [
                        s["StepName"]
                        for s in res["PipelineExecutionSteps"]
                        if s["StepStatus"] == "Executing"
                    ]
                    self.log.info("State of the pipeline steps: %s", count_by_state)
                    self.log.info("Steps currently in progress: %s", running_steps)

                    await asyncio.sleep(int(self.waiter_delay))

            raise AirflowException("Waiter error: max attempts reached")
