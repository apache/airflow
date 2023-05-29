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

from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class EmrAddStepsTrigger(BaseTrigger):
    """AWS Emr Add Steps Trigger"""

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

    @cached_property
    def hook(self) -> EmrHook:
        return EmrHook(aws_conn_id=self.aws_conn_id)

    async def run(self):
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
