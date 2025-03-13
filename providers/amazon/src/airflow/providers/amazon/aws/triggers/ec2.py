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

from airflow.providers.amazon.aws.hooks.ec2 import EC2Hook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class EC2StateSensorTrigger(BaseTrigger):
    """
    Poll the EC2 instance and yield a TriggerEvent once the state of the instance matches the target_state.

    :param instance_id: id of the AWS EC2 instance
    :param target_state: target state of instance
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: (optional) aws region name associated with the client
    :param poll_interval: number of seconds to wait before attempting the next poll
    """

    def __init__(
        self,
        instance_id: str,
        target_state: str,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
        poll_interval: int = 60,
    ):
        self.instance_id = instance_id
        self.target_state = target_state
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.ec2.EC2StateSensorTrigger",
            {
                "instance_id": self.instance_id,
                "target_state": self.target_state,
                "aws_conn_id": self.aws_conn_id,
                "region_name": self.region_name,
                "poll_interval": self.poll_interval,
            },
        )

    @cached_property
    def hook(self):
        return EC2Hook(aws_conn_id=self.aws_conn_id, region_name=self.region_name, api_type="client_type")

    async def run(self):
        while True:
            instance_state = await self.hook.get_instance_state_async(instance_id=self.instance_id)
            self.log.info("instance state: %s", instance_state)
            if instance_state == self.target_state:
                yield TriggerEvent({"status": "success", "message": "target state met"})
                break
            else:
                await asyncio.sleep(self.poll_interval)
