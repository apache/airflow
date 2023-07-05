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
from typing import Any

from botocore.exceptions import WaiterError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook, EmrHook, EmrServerlessHook
from airflow.providers.amazon.aws.utils.waiter_with_logging import async_wait
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook, EmrHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook, EmrHook, EmrServerlessHook
from airflow.providers.amazon.aws.utils.waiter_with_logging import async_wait
from airflow.triggers.base import BaseTrigger, TriggerEvent


class EmrAddStepsTrigger(BaseTrigger):
    """
    Asynchronously poll the boto3 API and wait for the steps to finish executing.

    :param job_flow_id: The id of the job flow.
    :param step_ids: The id of the steps being waited upon.
    :param poll_interval: The amount of time in seconds to wait between attempts.
    :param max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        job_flow_id: str,
        step_ids: list[str],
        aws_conn_id: str,
        max_attempts: int | None,
        poll_interval: int | None,
    ):
        self.job_flow_id = job_flow_id
        self.step_ids = step_ids
        self.aws_conn_id = aws_conn_id
        self.max_attempts = max_attempts
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.emr.EmrAddStepsTrigger",
            {
                "job_flow_id": str(self.job_flow_id),
                "step_ids": self.step_ids,
                "poll_interval": str(self.poll_interval),
                "max_attempts": str(self.max_attempts),
                "aws_conn_id": str(self.aws_conn_id),
            },
        )

    async def run(self):
        self.hook = EmrHook(aws_conn_id=self.aws_conn_id)
        async with self.hook.async_conn as client:
            for step_id in self.step_ids:
                attempt = 0
                waiter = client.get_waiter("step_complete")
                while attempt < int(self.max_attempts):
                    attempt += 1
                    try:
                        await waiter.wait(
                            ClusterId=self.job_flow_id,
                            StepId=step_id,
                            WaiterConfig={
                                "Delay": int(self.poll_interval),
                                "MaxAttempts": 1,
                            },
                        )
                        break
                    except WaiterError as error:
                        if "terminal failure" in str(error):
                            yield TriggerEvent(
                                {"status": "failure", "message": f"Step {step_id} failed: {error}"}
                            )
                            break
                        self.log.info(
                            "Status of step is %s - %s",
                            error.last_response["Step"]["Status"]["State"],
                            error.last_response["Step"]["Status"]["StateChangeReason"],
                        )
                        await asyncio.sleep(int(self.poll_interval))
        if attempt >= int(self.max_attempts):
            yield TriggerEvent({"status": "failure", "message": "Steps failed: max attempts reached"})
        else:
            yield TriggerEvent({"status": "success", "message": "Steps completed", "step_ids": self.step_ids})


class EmrCreateJobFlowTrigger(AwsBaseWaiterTrigger):
    """
    Asynchronously poll the boto3 API and wait for the JobFlow to finish executing.

    :param job_flow_id: The id of the job flow to wait for.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        job_flow_id: str,
        poll_interval: int | None = None,  # deprecated
        max_attempts: int | None = None,  # deprecated
        aws_conn_id: str | None = None,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
    ):
        if poll_interval is not None or max_attempts is not None:
            warnings.warn(
                "please use waiter_delay instead of poll_interval "
                "and waiter_max_attempts instead of max_attempts",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            waiter_delay = poll_interval or waiter_delay
            waiter_max_attempts = max_attempts or waiter_max_attempts
        super().__init__(
            serialized_fields={"job_flow_id": job_flow_id},
            waiter_name="job_flow_waiting",
            waiter_args={"ClusterId": job_flow_id},
            failure_message="JobFlow creation failed",
            status_message="JobFlow creation in progress",
            status_queries=[
                "Cluster.Status.State",
                "Cluster.Status.StateChangeReason",
                "Cluster.Status.ErrorDetails",
            ],
            return_key="job_flow_id",
            return_value=job_flow_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return EmrHook(aws_conn_id=self.aws_conn_id)


class EmrTerminateJobFlowTrigger(AwsBaseWaiterTrigger):
    """
    Asynchronously poll the boto3 API and wait for the JobFlow to finish terminating.

    :param job_flow_id: ID of the EMR Job Flow to terminate
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        job_flow_id: str,
        poll_interval: int | None = None,  # deprecated
        max_attempts: int | None = None,  # deprecated
        aws_conn_id: str | None = None,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
    ):
        if poll_interval is not None or max_attempts is not None:
            warnings.warn(
                "please use waiter_delay instead of poll_interval "
                "and waiter_max_attempts instead of max_attempts",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            waiter_delay = poll_interval or waiter_delay
            waiter_max_attempts = max_attempts or waiter_max_attempts
        super().__init__(
            serialized_fields={"job_flow_id": job_flow_id},
            waiter_name="job_flow_terminated",
            waiter_args={"ClusterId": job_flow_id},
            failure_message="JobFlow termination failed",
            status_message="JobFlow termination in progress",
            status_queries=[
                "Cluster.Status.State",
                "Cluster.Status.StateChangeReason",
                "Cluster.Status.ErrorDetails",
            ],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return EmrHook(aws_conn_id=self.aws_conn_id)


class EmrContainerTrigger(AwsBaseWaiterTrigger):
    """
    Poll for the status of EMR container until reaches terminal state.

    :param virtual_cluster_id: Reference Emr cluster id
    :param job_id:  job_id to check the state
    :param aws_conn_id: Reference to AWS connection id
    :param waiter_delay: polling period in seconds to check for the status
    """

    def __init__(
        self,
        virtual_cluster_id: str,
        job_id: str,
        aws_conn_id: str = "aws_default",
        poll_interval: int | None = None,  # deprecated
        waiter_delay: int = 30,
        waiter_max_attempts: int = 600,
    ):
        if poll_interval is not None:
            warnings.warn(
                "please use waiter_delay instead of poll_interval.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            waiter_delay = poll_interval or waiter_delay
        super().__init__(
            serialized_fields={"virtual_cluster_id": virtual_cluster_id, "job_id": job_id},
            waiter_name="container_job_complete",
            waiter_args={"id": job_id, "virtualClusterId": virtual_cluster_id},
            failure_message="Job failed",
            status_message="Job in progress",
            status_queries=["jobRun.state", "jobRun.failureReason"],
            return_key="job_id",
            return_value=job_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return EmrContainerHook(self.aws_conn_id)


class EmrStepSensorTrigger(AwsBaseWaiterTrigger):
    """
    Poll for the status of EMR container until reaches terminal state.

    :param job_flow_id: job_flow_id which contains the step check the state of
    :param step_id:  step to check the state of
    :param waiter_delay: polling period in seconds to check for the status
    :param waiter_max_attempts: The maximum number of attempts to be made
    :param aws_conn_id: Reference to AWS connection id
    """

    def __init__(
        self,
        job_flow_id: str,
        step_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str = "aws_default",
    ):
        super().__init__(
            serialized_fields={"job_flow_id": job_flow_id, "step_id": step_id},
            waiter_name="step_wait_for_terminal",
            waiter_args={"ClusterId": job_flow_id, "StepId": step_id},
            failure_message=f"Error while waiting for step {step_id} to complete",
            status_message=f"Step id: {step_id}, Step is still in non-terminal state",
            status_queries=[
                "Step.Status.State",
                "Step.Status.FailureDetails",
                "Step.Status.StateChangeReason",
            ],
            return_value=None,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:

        async with self.hook.async_conn as client:
            waiter = client.get_waiter("step_wait_for_terminal", deferrable=True, client=client)
            await async_wait(
                waiter=waiter,
                waiter_delay=self.poke_interval,
                waiter_max_attempts=self.max_attempts,
                args={"ClusterId": self.job_flow_id, "StepId": self.step_id},
                failure_message=f"Error while waiting for step {self.step_id} to complete",
                status_message=f"Step id: {self.step_id}, Step is still in non-terminal state",
                status_args=["Step.Status.State"],
            )

        yield TriggerEvent({"status": "success"})


class EmrServerlessAppicationTrigger(BaseTrigger):
    """
    Trigger for Emr Serverless applications.

    This Trigger will asynchronously poll for the status of EMR Serverless Application
    until it reaches a particular state, which is determined by the waiter_name.

    :param application_id: EMR Serverless application ID
    :param waiter_name: Name of the waiter to use to wait for the application to complete
    :param aws_conn_id: Reference to AWS connection id
    :param waiter_delay: Delay in seconds between each attempt to check the status
    :param waiter_max_attempts: Maximum number of attempts to check the status
    """

    def __init__(
        self,
        application_id: str,
        waiter_name: str,
        aws_conn_id: str,
        waiter_delay: int,
        waiter_max_attempts: int,
    ):
        self.application_id = application_id
        self.waiter_name = waiter_name
        self.aws_conn_id = aws_conn_id
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.emr.EmrServerlessAppicationTrigger",
            {
                "application_id": self.application_id,
                "waiter_name": self.waiter_name,
                "aws_conn_id": self.aws_conn_id,
                "waiter_delay": str(self.waiter_delay),
                "waiter_max_attempts": str(self.waiter_max_attempts),
            },
        )

    async def run(self):
        self.hook = EmrServerlessHook(aws_conn_id=self.aws_conn_id)
        async with self.hook.async_conn as client:
            waiter = self.hook.get_waiter(self.waiter_name, deferrable=True, client=client)
            await async_wait(
                waiter=waiter,
                waiter_delay=self.waiter_delay,
                waiter_max_attempts=self.waiter_max_attempts,
                args={"applicationId": self.application_id},
                failure_message=f"Error while waiting for application {self.application_id} to complete",
                status_message="Status of EMR Serverless Application is",
                status_args=["application.state", "application.stateDetails"],
            )
        yield TriggerEvent({"status": "success", "application_id": self.application_id})


class EmrServerlessCancelJobsTrigger(BaseTrigger):
    """
    Trigger for canceling a list of jobs in an EMR Serverless application.

    :param application_id: EMR Serverless application ID
    :param aws_conn_id: Reference to AWS connection id
    :param waiter_delay: Delay in seconds between each attempt to check the status
    :param waiter_max_attempts: Maximum number of attempts to check the status
    """

    def __init__(
        self,
        application_id: str,
        aws_conn_id: str,
        waiter_delay: int,
        waiter_max_attempts: int,
    ):
        self.application_id = application_id
        self.aws_conn_id = aws_conn_id
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.emr.EmrServerlessCancelJobsTrigger",
            {
                "application_id": self.application_id,
                "aws_conn_id": self.aws_conn_id,
                "waiter_delay": str(self.waiter_delay),
                "waiter_max_attempts": str(self.waiter_max_attempts),
            },
        )

    async def run(self):
        self.hook = EmrServerlessHook(aws_conn_id=self.aws_conn_id)
        self.log.info("Waiting for jobs to cancel")

        states = list(self.hook.JOB_INTERMEDIATE_STATES.union({"CANCELLING"}))

        async with self.hook.async_conn as client:
            waiter = self.hook.get_waiter("no_job_running", deferrable=True, client=client)
            await async_wait(
                waiter=waiter,
                waiter_delay=self.waiter_delay,
                waiter_max_attempts=self.waiter_max_attempts,
                args={"applicationId": self.application_id, "states": states},
                failure_message="Error while waiting for jobs to cancel",
                status_message="Currently running jobs",
                status_args=["jobRuns[*].applicationId", "jobRuns[*].state"],
            )
        yield TriggerEvent({"status": "success", "application_id": self.application_id})
