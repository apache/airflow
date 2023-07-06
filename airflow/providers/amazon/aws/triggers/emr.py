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
from functools import cached_property
from typing import Any, AsyncIterator

from botocore.exceptions import WaiterError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook, EmrHook, EmrServerlessHook
from airflow.providers.amazon.aws.utils.waiter_with_logging import async_wait
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.helpers import prune_dict


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


class EmrCreateJobFlowTrigger(BaseTrigger):
    """
    Asynchronously poll the boto3 API and wait for the JobFlow to finish executing.

    :param job_flow_id: The id of the job flow to wait for.
    :param poll_interval: The amount of time in seconds to wait between attempts.
    :param max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        job_flow_id: str,
        poll_interval: int,
        max_attempts: int,
        aws_conn_id: str,
    ):
        self.job_flow_id = job_flow_id
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "job_flow_id": self.job_flow_id,
                "poll_interval": str(self.poll_interval),
                "max_attempts": str(self.max_attempts),
                "aws_conn_id": self.aws_conn_id,
            },
        )

    async def run(self):
        self.hook = EmrHook(aws_conn_id=self.aws_conn_id)
        async with self.hook.async_conn as client:
            attempt = 0
            waiter = self.hook.get_waiter("job_flow_waiting", deferrable=True, client=client)
            while attempt < int(self.max_attempts):
                attempt = attempt + 1
                try:
                    await waiter.wait(
                        ClusterId=self.job_flow_id,
                        WaiterConfig=prune_dict(
                            {
                                "Delay": self.poll_interval,
                                "MaxAttempts": 1,
                            }
                        ),
                    )
                    break
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        raise AirflowException(f"JobFlow creation failed: {error}")
                    self.log.info(
                        "Status of jobflow is %s - %s",
                        error.last_response["Cluster"]["Status"]["State"],
                        error.last_response["Cluster"]["Status"]["StateChangeReason"],
                    )
                    await asyncio.sleep(int(self.poll_interval))
        if attempt >= int(self.max_attempts):
            raise AirflowException(f"JobFlow creation failed - max attempts reached: {self.max_attempts}")
        else:
            yield TriggerEvent(
                {
                    "status": "success",
                    "message": "JobFlow completed successfully",
                    "job_flow_id": self.job_flow_id,
                }
            )


class EmrTerminateJobFlowTrigger(BaseTrigger):
    """
    Asynchronously poll the boto3 API and wait for the JobFlow to finish terminating.

    :param job_flow_id: ID of the EMR Job Flow to terminate
    :param poll_interval: The amount of time in seconds to wait between attempts.
    :param max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        job_flow_id: str,
        poll_interval: int,
        max_attempts: int,
        aws_conn_id: str,
    ):
        self.job_flow_id = job_flow_id
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "job_flow_id": self.job_flow_id,
                "poll_interval": str(self.poll_interval),
                "max_attempts": str(self.max_attempts),
                "aws_conn_id": self.aws_conn_id,
            },
        )

    async def run(self):
        self.hook = EmrHook(aws_conn_id=self.aws_conn_id)
        async with self.hook.async_conn as client:
            attempt = 0
            waiter = self.hook.get_waiter("job_flow_terminated", deferrable=True, client=client)
            while attempt < int(self.max_attempts):
                attempt = attempt + 1
                try:
                    await waiter.wait(
                        ClusterId=self.job_flow_id,
                        WaiterConfig=prune_dict(
                            {
                                "Delay": self.poll_interval,
                                "MaxAttempts": 1,
                            }
                        ),
                    )
                    break
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        raise AirflowException(f"JobFlow termination failed: {error}")
                    self.log.info(
                        "Status of jobflow is %s - %s",
                        error.last_response["Cluster"]["Status"]["State"],
                        error.last_response["Cluster"]["Status"]["StateChangeReason"],
                    )
                    await asyncio.sleep(int(self.poll_interval))
        if attempt >= int(self.max_attempts):
            raise AirflowException(f"JobFlow termination failed - max attempts reached: {self.max_attempts}")
        else:
            yield TriggerEvent(
                {
                    "status": "success",
                    "message": "JobFlow terminated successfully",
                }
            )


class EmrContainerTrigger(BaseTrigger):
    """
    Poll for the status of EMR container until reaches terminal state.

    :param virtual_cluster_id: Reference Emr cluster id
    :param job_id:  job_id to check the state
    :param aws_conn_id: Reference to AWS connection id
    :param poll_interval: polling period in seconds to check for the status
    """

    def __init__(
        self,
        virtual_cluster_id: str,
        job_id: str,
        aws_conn_id: str = "aws_default",
        poll_interval: int = 30,
        **kwargs: Any,
    ):
        self.virtual_cluster_id = virtual_cluster_id
        self.job_id = job_id
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval
        super().__init__(**kwargs)

    @cached_property
    def hook(self) -> EmrContainerHook:
        return EmrContainerHook(self.aws_conn_id, virtual_cluster_id=self.virtual_cluster_id)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes EmrContainerTrigger arguments and classpath."""
        return (
            "airflow.providers.amazon.aws.triggers.emr.EmrContainerTrigger",
            {
                "virtual_cluster_id": self.virtual_cluster_id,
                "job_id": self.job_id,
                "aws_conn_id": self.aws_conn_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        async with self.hook.async_conn as client:
            waiter = self.hook.get_waiter("container_job_complete", deferrable=True, client=client)
            attempt = 0
            while True:
                attempt = attempt + 1
                try:
                    await waiter.wait(
                        id=self.job_id,
                        virtualClusterId=self.virtual_cluster_id,
                        WaiterConfig={
                            "Delay": self.poll_interval,
                            "MaxAttempts": 1,
                        },
                    )
                    break
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        yield TriggerEvent({"status": "failure", "message": f"Job Failed: {error}"})
                        break
                    self.log.info(
                        "Job status is %s. Retrying attempt %s",
                        error.last_response["jobRun"]["state"],
                        attempt,
                    )
                    await asyncio.sleep(int(self.poll_interval))

            yield TriggerEvent({"status": "success", "job_id": self.job_id})


class EmrStepSensorTrigger(BaseTrigger):
    """
    Poll for the status of EMR container until reaches terminal state.

    :param job_flow_id: job_flow_id which contains the step check the state of
    :param step_id:  step to check the state of
    :param aws_conn_id: Reference to AWS connection id
    :param max_attempts: The maximum number of attempts to be made
    :param poke_interval: polling period in seconds to check for the status
    """

    def __init__(
        self,
        job_flow_id: str,
        step_id: str,
        aws_conn_id: str = "aws_default",
        max_attempts: int = 60,
        poke_interval: int = 30,
        **kwargs: Any,
    ):
        self.job_flow_id = job_flow_id
        self.step_id = step_id
        self.aws_conn_id = aws_conn_id
        self.max_attempts = max_attempts
        self.poke_interval = poke_interval
        super().__init__(**kwargs)

    @cached_property
    def hook(self) -> EmrHook:
        return EmrHook(self.aws_conn_id)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.emr.EmrStepSensorTrigger",
            {
                "job_flow_id": self.job_flow_id,
                "step_id": self.step_id,
                "aws_conn_id": self.aws_conn_id,
                "max_attempts": self.max_attempts,
                "poke_interval": self.poke_interval,
            },
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
    Trigger for cancelling a list of jobs in an EMR Serverless application.

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
