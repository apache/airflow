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

from typing import Any

from airflow.compat.functools import cached_property
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class SageMakerTrigger(BaseTrigger):
    """
    SageMakerTrigger is common trigger for both transform and training sagemaker job, and it is
     fired as deferred class with params to run the task in triggerer.

    :param job_name: name of the job to check status
    :param job_type: Type of the sagemaker job whether it is Transform or Training
    :param poke_interval:  polling period in seconds to check for the status
    :param max_retries: Number of times to poll for query state before returning the current state,
        defaults to None.
    :param aws_conn_id: AWS connection ID for sagemaker
    """

    def __init__(
        self,
        job_name: str,
        job_type: str,
        poke_interval: int = 30,
        max_retries: int | None = None,
        aws_conn_id: str = "aws_default",
    ):
        super().__init__()
        self.job_name = job_name
        self.job_type = job_type
        self.poke_interval = poke_interval
        self.max_retries = max_retries
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes SagemakerTrigger arguments and classpath."""
        return (
            "airflow.providers.amazon.aws.triggers.sagemaker.SagemakerTrigger",
            {
                "job_name": self.job_name,
                "job_type": self.job_type,
                "poke_interval": self.poke_interval,
                "max_retries": self.max_retries,
                "aws_conn_id": self.aws_conn_id,
            },
        )

    @cached_property
    def hook(self) -> SageMakerHook:
        return SageMakerHook(aws_conn_id=self.aws_conn_id)

    async def run(self):
        self.log.info("job name is %s", self.job_name)
        async with self.hook.async_conn as client:
            waiter = self.hook.get_waiter("TrainingJobComplete", deferrable=True, client=client)
            await waiter.wait(
                TrainingJobName=self.job_name,
                WaiterConfig={
                    "Delay": self.poke_interval,
                    "MaxAttempts": self.max_retries,
                },
            )
        yield TriggerEvent({"status": "success", "message": "Job completed."})
