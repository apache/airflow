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

from airflow.providers.amazon.aws.hooks.ecs import EcsHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class ClusterActiveTrigger(BaseTrigger):
    """
    Waits for a cluster to be active, triggers when it finishes

    :param cluster_arn: ARN of the cluster to watch.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The number of times to ping for status.
        Will fail after that many unsuccessful attempts.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region: The AWS region where the cluster is located.
    """

    def __init__(
        self,
        cluster_arn: str,
        waiter_delay: int | None,
        waiter_max_attempts: int | None,
        aws_conn_id: str | None,
        region: str | None,
    ):
        self.cluster_arn = cluster_arn
        self.waiter_delay = waiter_delay or 15
        self.attempts = waiter_max_attempts or 999999999
        self.aws_conn_id = aws_conn_id
        self.region = region

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "cluster_arn": self.cluster_arn,
                "waiter_delay": self.waiter_delay,
                "waiter_max_attempts": self.attempts,
                "aws_conn_id": self.aws_conn_id,
                "region": self.region,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = EcsHook(aws_conn_id=self.aws_conn_id, region_name=self.region)
        async with hook.async_conn as client:
            waiter = hook.get_waiter("cluster_active", deferrable=True, client=client)
            while self.attempts >= 1:
                self.attempts = self.attempts - 1
                try:
                    waiter.wait(
                        clusters=[self.cluster_arn],
                        WaiterConfig={
                            "MaxAttempts": 1,
                        },
                    )
                    break  # we reach this point only if the waiter met a success criteria
                except WaiterError as error:
                    if "terminal failure" in str(error):
                        raise
                    self.log.info("Status of cluster is %s", error.last_response["clusters"][0]["status"])
                    await asyncio.sleep(int(self.waiter_delay))

        yield TriggerEvent({"status": "success", "value": self.cluster_arn})
