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
from typing import Any, AsyncIterator

from botocore.exceptions import WaiterError

from airflow.compat.functools import cached_property
from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class EmrContainerOperatorTrigger(BaseTrigger):
    """
    Poll for the status of EMR container until reaches terminal state

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
        poll_interval: int = 10,
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
        """Serializes EmrContainerOperatorTrigger arguments and classpath."""
        return (
            "airflow.providers.amazon.aws.triggers.emr.EmrContainerOperatorTrigger",
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
