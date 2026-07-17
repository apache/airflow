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
import warnings
from collections import Counter
from collections.abc import AsyncIterator
from enum import IntEnum
from typing import TYPE_CHECKING

from botocore.exceptions import WaiterError

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger
from airflow.providers.common.compat.sdk import AirflowException
from airflow.triggers.base import TriggerEvent

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class SageMakerTrigger(AwsBaseWaiterTrigger):
    """
    SageMakerTrigger is fired as deferred class with params to run the task in triggerer.

    :param job_name: name of the job to check status
    :param job_type: Type of the sagemaker job whether it is Transform or Training
    :param waiter_delay: polling period in seconds to check for the status
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: AWS connection ID for sagemaker
    :param region_name: The AWS region where the job is running. Used to build the hook.
    :param verify: Whether or not to verify SSL certificates. Used to build the hook.
    :param botocore_config: Configuration dictionary for the botocore client. Used to build the hook.
    :param poke_interval: (deprecated) use ``waiter_delay`` instead.
    :param max_attempts: (deprecated) use ``waiter_max_attempts`` instead.
    """

    def __init__(
        self,
        job_name: str,
        job_type: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 480,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
        poke_interval: int | None = None,
        max_attempts: int | None = None,
    ):
        if poke_interval is not None:
            warnings.warn(
                "`poke_interval` is deprecated and will be removed in a future release. "
                "Please use `waiter_delay` instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            waiter_delay = poke_interval
        if max_attempts is not None:
            warnings.warn(
                "`max_attempts` is deprecated and will be removed in a future release. "
                "Please use `waiter_max_attempts` instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            waiter_max_attempts = max_attempts
        self.job_name = job_name
        self.job_type = job_type
        super().__init__(
            serialized_fields={"job_name": job_name, "job_type": job_type},
            waiter_name=self._get_job_type_waiter(job_type),
            waiter_args={self._get_waiter_arg_name(job_type): job_name},
            failure_message=f"Error while waiting for {job_type} job",
            status_message=f"{job_type} job not done yet",
            status_queries=[self._get_response_status_key(job_type)],
            return_key="job_name",
            return_value=job_name,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
            verify=verify,
            botocore_config=botocore_config,
        )

    def hook(self) -> AwsGenericHook:
        return SageMakerHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )

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


class SageMakerPipelineTrigger(AwsBaseWaiterTrigger):
    """
    Trigger to wait for a sagemaker pipeline execution to finish.

    :param waiter_type: Type of waiter to use, see ``Type`` enum.
    :param pipeline_execution_arn: ARN of the pipeline execution to wait for.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: The AWS region where the pipeline runs. Used to build the hook.
    :param verify: Whether or not to verify SSL certificates. Used to build the hook.
    :param botocore_config: Configuration dictionary for the botocore client. Used to build the hook.
    """

    class Type(IntEnum):
        """Type of waiter to use."""

        COMPLETE = 1
        STOPPED = 2

    _waiter_name = {
        Type.COMPLETE: "PipelineExecutionComplete",
        Type.STOPPED: "PipelineExecutionStopped",
    }

    def __init__(
        self,
        waiter_type: Type | int,
        pipeline_execution_arn: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str | None,
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
    ):
        # waiter_type arrives as an int when deserialized from a serialized trigger.
        self.waiter_type = self.Type(waiter_type)
        self.pipeline_execution_arn = pipeline_execution_arn
        super().__init__(
            serialized_fields={
                "waiter_type": self.waiter_type.value,  # saving the int value here
                "pipeline_execution_arn": pipeline_execution_arn,
            },
            waiter_name=self._waiter_name[self.waiter_type],
            waiter_args={"PipelineExecutionArn": pipeline_execution_arn},
            failure_message="Error while waiting for the pipeline execution to finish",
            status_message="Pipeline execution not done yet",
            status_queries=["PipelineExecutionStatus"],
            return_value=pipeline_execution_arn,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
            verify=verify,
            botocore_config=botocore_config,
        )

    def hook(self) -> AwsGenericHook:
        return SageMakerHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        # Custom polling loop (instead of the base waiter loop) so we can surface
        # per-step pipeline progress in the logs between attempts.
        hook = self.hook()
        async with await hook.get_async_conn() as conn:
            waiter = hook.get_waiter(self.waiter_name, deferrable=True, client=conn)
            for _ in range(self.attempts):
                try:
                    await waiter.wait(
                        PipelineExecutionArn=self.pipeline_execution_arn, WaiterConfig={"MaxAttempts": 1}
                    )
                    # we reach this point only if the waiter met a success criteria
                    yield TriggerEvent({"status": "success", "value": self.pipeline_execution_arn})
                    return
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        raise

                    self.log.info(
                        "Status of the pipeline execution: %s", error.last_response["PipelineExecutionStatus"]
                    )

                    res = await conn.list_pipeline_execution_steps(
                        PipelineExecutionArn=self.pipeline_execution_arn
                    )
                    count_by_state = Counter(s["StepStatus"] for s in res["PipelineExecutionSteps"])
                    running_steps = [
                        s["StepName"] for s in res["PipelineExecutionSteps"] if s["StepStatus"] == "Executing"
                    ]
                    self.log.info("State of the pipeline steps: %s", count_by_state)
                    self.log.info("Steps currently in progress: %s", running_steps)

                    await asyncio.sleep(int(self.waiter_delay))

            raise AirflowException("Waiter error: max attempts reached")
