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
from typing import Any

from botocore.exceptions import WaiterError

from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class BatchOperatorTrigger(BaseTrigger):
    """
    Trigger for BatchOperator.
    The trigger will asynchronously poll the boto3 API and wait for the
    Batch job to be in the `SUCCEEDED` state.

    :param job_id:  A unique identifier for the cluster.
    :param max_retries: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: region name to use in AWS Hook
    :param poll_interval: The amount of time in seconds to wait between attempts.
    """

    def __init__(
        self,
        job_id: str | None = None,
        max_retries: int = 10,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        poll_interval: int = 30,
    ):
        super().__init__()
        self.job_id = job_id
        self.max_retries = max_retries
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes BatchOperatorTrigger arguments and classpath."""
        return (
            "airflow.providers.amazon.aws.triggers.batch.BatchOperatorTrigger",
            {
                "job_id": self.job_id,
                "max_retries": self.max_retries,
                "aws_conn_id": self.aws_conn_id,
                "region_name": self.region_name,
                "poll_interval": self.poll_interval,
            },
        )

    @cached_property
    def hook(self) -> BatchClientHook:
        return BatchClientHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

    async def run(self):

        async with self.hook.async_conn as client:
            waiter = self.hook.get_waiter("batch_job_complete", deferrable=True, client=client)
            attempt = 0
            while attempt < self.max_retries:
                attempt = attempt + 1
                try:
                    await waiter.wait(
                        jobs=[self.job_id],
                        WaiterConfig={
                            "Delay": self.poll_interval,
                            "MaxAttempts": 1,
                        },
                    )
                    break
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        yield TriggerEvent(
                            {"status": "failure", "message": f"Delete Cluster Failed: {error}"}
                        )
                        break
                    self.log.info(
                        "Job status is %s. Retrying attempt %s/%s",
                        error.last_response["jobs"][0]["status"],
                        attempt,
                        self.max_retries,
                    )
                    await asyncio.sleep(int(self.poll_interval))

        if attempt >= self.max_retries:
            yield TriggerEvent({"status": "failure", "message": "Job Failed - max attempts reached."})
        else:
            yield TriggerEvent({"status": "success", "job_id": self.job_id})


class BatchSensorTrigger(BaseTrigger):
    """
    Checks for the status of a submitted job_id to AWS Batch until it reaches a failure or a success state.
    BatchSensorTrigger is fired as deferred class with params to poll the job state in Triggerer.

    :param job_id: the job ID, to poll for job completion or not
    :param region_name: AWS region name to use
        Override the region_name in connection (if provided)
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used
    :param poke_interval: polling period in seconds to check for the status of the job
    """

    def __init__(
        self,
        job_id: str,
        region_name: str | None,
        aws_conn_id: str | None = "aws_default",
        poke_interval: float = 5,
    ):
        super().__init__()
        self.job_id = job_id
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes BatchSensorTrigger arguments and classpath."""
        return (
            "airflow.providers.amazon.aws.triggers.batch.BatchSensorTrigger",
            {
                "job_id": self.job_id,
                "aws_conn_id": self.aws_conn_id,
                "region_name": self.region_name,
                "poke_interval": self.poke_interval,
            },
        )

    @cached_property
    def hook(self) -> BatchClientHook:
        return BatchClientHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

    async def run(self):
        """
        Make async connection using aiobotocore library to AWS Batch,
        periodically poll for the Batch job status.

        The status that indicates job completion are: 'SUCCEEDED'|'FAILED'.
        """
        async with self.hook.async_conn as client:
            waiter = self.hook.get_waiter("batch_job_complete", deferrable=True, client=client)
            attempt = 0
            while True:
                attempt = attempt + 1
                try:
                    await waiter.wait(
                        jobs=[self.job_id],
                        WaiterConfig={
                            "Delay": int(self.poke_interval),
                            "MaxAttempts": 1,
                        },
                    )
                    break
                except WaiterError as error:
                    if "error" in str(error):
                        yield TriggerEvent({"status": "failure", "message": f"Job Failed: {error}"})
                        break
                    self.log.info(
                        "Job response is %s. Retrying attempt %s",
                        error.last_response["Error"]["Message"],
                        attempt,
                    )
                    await asyncio.sleep(int(self.poke_interval))

            yield TriggerEvent(
                {
                    "status": "success",
                    "job_id": self.job_id,
                    "message": f"Job {self.job_id} Succeeded",
                }
            )
